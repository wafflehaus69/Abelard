# BARREL M0 — Paid-Month Runbook v1 (operational)

**Author:** ClaudeCode · **Date:** 2026-10-01 · **Returns:** the Architect's launch order (`RUNBOOK_ARCHITECT_2026-10-01.md`, MR-13) with the operational detail filled in.
**Status:** revised after MR-14 (`RULINGS_2026-10-01_C.md`). **Not ready: one blocker, §1.**
**Plan assumed:** Analyst, month-to-month, 4,000 credits a month, 3,400 spendable after the 15% reserve, two months authorized. Under the Plus rule the same phases run uncut in one month.

---

## 1. Status before purchase

**Not ready. One blocker.** Everything the Architect ordered before purchase is done except that order 2 came back with the result it was written to catch.

| Order (MR-14) | Result |
|---|---|
| 1. Grouped `b1a`, one day, ≤ 60 | **Done, 46.2 credits.** Member and grouped forms agree on set size, holdings, flows and supply on 236 of 236 tokens, and on funders on 235. The one difference is two senders in the same second; the query now breaks ties by slot. That fix has not been run. `B1A_GROUPED_VALIDATION.md` |
| 2. Virtual reserve over RPC | **HALT.** Not one constant: 10 of 31 post-BOOST pools carry 0 (mayhem-mode pools), 21 carry about 17.58 SOL. Fixed for a pool's life. A derivation from each pool's own buys matched the accounts on 35 of 35. `VQR_CHECK.md` |
| 3. Threshold grep in the runner | **Done.** `recon/verdict_constants.py`; no override. The burned-week query is refused (regression test), all 76 scheduled chunk files pass. |
| 4. Null-count item for the first paid chunk | **Recorded.** `recon/sql/buy_field_swap_by_day.sql`, day 0 step 5. |
| 5. Depth floor against withdrawable reserve | **Done.** `M0_TASKING.md` MR-14; the dry run is struck. |
| 6. "Ready", and the day | **Ready on the day the halt is lifted.** That needs the Architect's acceptance of the derived reserve and one post-BOOST day of the regenerated event query (30–50 trial credits, hard cap, not yet authorized; 165 are spendable until 2026-10-06). If both come today, day 0 can be **2026-10-02**. |

**Also open:** ruling 6 mentions "one addition for Mando below"; none was in the text relayed.

### Rulings applied (MR-14)

| # | Ruling | Where it is now |
|---|---|---|
| 1, 8 | 19 chunks; window end 2026-09-20 signed | §4 |
| 2 | Phase A 400 / 500 from C; runs the whole 2026-08 chunk including `b1a` | §5 |
| 3 | In-warehouse expected on Analyst; day 0 measures the rate; **`b1a` output always exported, never materialized** | §5 day 0, §7 C3 |
| 4 | Sampled window as proposed | §7 C1 |
| 5 | Definitions may live in a query, verdicts may not; the bundle's five is applied locally; runner refuses registered constants | §2 |
| 6 | Rows to gitignored `barrel/data/`; manifest committed the same day | §2, §5 |
| 7 | Grouped `b1a` one-day run | done, above |

---

## 2. Standing rules, and what enforces each

| Rule | Enforced by |
|---|---|
| Expected cost is the hard cap for an unproven pattern | `recon/dune_run_sql.py`: `--expect` is the cap unless `--proven`; above 25 needs `--confirm` |
| New patterns at one-day scope first | every generator writes a one-day file; the manifest marks which patterns are proven (§4) |
| Null-count every input column per era | `recon/sql/*_probe.sql`; one probe per chunk era before its first run (§5 step 1) |
| Cost runs do not fetch rows | `--no-rows` |
| Rows that name wallets stay out of the repo | `--private-rows`; owner wallets dropped from any fetched row |
| 15% reserve | the runner refuses a run that would cross it, and never below 180 absolute |
| Daily ledger | `docs/credits.md`, one block per day (§6) |
| No verdict threshold in any query, saved or not | `recon/verdict_constants.py`: the runner refuses a query matching a registered constant; no override. The chunk review applies the same list. New thresholds are registered in the commit that records their ruling. |
| Exports | rows to gitignored `barrel/data/`; SHA-256, row count and per-column null count per file in a committed manifest the same day. `b1a` groups are always exported and never materialized. |
| Owner wallets never in a saved query | `__NOT_OWNER(col)__` is substituted at run time from `barrel/private/`; the chunk review fails any wallet-bearing query without it |
| In-flight counter is not a meter | It jumps by up to 56 credits and has read above the final bill. Cancellation bounds slow overruns only; the one-day rule is the protection. |

## 3. Pre-purchase checklist

| Item | State |
|---|---|
| Post-BOOST price-path week, burned, by seed, recorded first | **Done.** 44.05 of 50. `PRICEPATH_BURNED_WEEK.md`. Week 2026-08-10 → 08-16. |
| `PURCHASE_DECISION.md` with both paths | **Done** (third version) and amended for the 19-chunk list. |
| Chunk list, queries generated and reviewed, zero credits | **Done.** 19 chunks, 95 files in `recon/sql/chunks/`, `recon/chunks_manifest.json`, review problems 0. |
| `RUNBOOK_v1.md` | this document |
| Defects found on the way | `ix_name` is NULL on every buy event: gross and net were mis-assigned on up to 45% of 2026 buys. Fixed in the generator; fees re-measured. |

Trial balance: 2,100.4 used; **219.6 spendable** above the reserve until 2026-10-06.

---

## 4. Chunk list

One calendar month of stratum-P graduations per chunk. Files: `recon/sql/chunks/<chunk>_<query>.sql`.

| Query | Feeds | Proven at |
|---|---|---|
| `events` | the event-derived columns | day and week, pre-BOOST; week post-BOOST in part (price columns) |
| `b1a` | aligned set grouped by funder: c07, c08, `seta_*`, `seta_lat_s`, `blk`, registry | **member form** day, week, post-BOOST day. **Grouped form: not run.** |
| `b1b` | c06, supply at entry | day, week, post-BOOST day |
| `b2` | c01, c02, `token_program` | day, week, post-BOOST day |
| `b3` | `cluster_*` (H2). Generated, **not scheduled on Analyst** | day, week, post-BOOST day |

Projected credits per chunk (measured units: a fixed part plus a rate per day of large table scanned, times the post-BOOST ratio for post-BOOST days):

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

**4,808 credits for the core build**, against 6,800 spendable over two Analyst months. If every chunk costs the post-BOOST rate, about 6,200. Neither figure includes getting the table out (§1 point 3).

What the static review checks on every file, and found nothing wrong with: each large table carries literal partition bounds; no large table is referenced more often than designed (SOL transfers three times in `b1a`, twice in `b3`; token ledger once; swap events once); wallet-bearing queries carry the owner filter; the chunk's dates are in the text; no `OR` on a partition column; `SELECT` only. It cannot show a query is cheap. Only the one-day run does.

---

## 5. Phases

Order of chunks in every phase: **2026-08 first** (phase A), then post-BOOST and mixed (2026-09, 2026-07), then backwards from 2026-06 to 2025-03. The newest eras are the least measured, so their cost is learned first.

### Day 0 — at purchase, before any build query
1. Read at checkout: monthly price, overage policy, export rate. Record in `credits.md`.
2. If overage bills automatically: set the runner's allowance to the plan's credits so it stops at the reserve.
3. **Export rate, measured:** one fetch of about 1 MB from a kept trial result. Decides export-first or in-warehouse for the whole month.
4. **Virtual reserve: done before purchase, and it halted** (`VQR_CHECK.md`). Day 0 repeats the free RPC read on 30 fresh pools and compares them with the reserve the event query derives for the same pools. Any mismatch is C5.
5. **Null-count probes, about 10 credits:** one hour per era for every input column of `events`, `b1a`, `b1b`, `b2`; and `recon/sql/buy_field_swap_by_day.sql` (MR-14 order 4): per day from 2026-08-08 to 09-02, how many buys have each quote field the larger, on stratum-P pools and on all pools, to explain why the burned week and the single sampled days disagree.

### Phase A — scaling check (day 1)
| Step | File | Cap |
|---|---|---|
| A0 | `events`, one post-BOOST day, with the per-pool derived reserve (skipped if run on the trial) | 50, hard |
| A1 | `heavy_b1a_grouped_pbday.sql`: grouped form with the slot tie-break, one post-BOOST day | 70, hard |
| A2 | `chunks/2026-08_events.sql` | 90, hard |
| A3 | `chunks/2026-08_b1b.sql`, `2026-08_b2.sql` | 35 and 5 |
| A4 | `chunks/2026-08_b1a.sql` | 320, hard |

**Budget: 400 / 500** (MR-14 ruling 2), taken from phase C. **Gate:** measured cost of the 2026-08 chunk ≤ 1.3 × 338 = **440**. Above that → C1. The gate is on the chunk; A0 and A1 are inside the 500 ceiling but outside the gate.
**Also checked here:** A1's grouped rows against the member rows already on disk for 2026-09-01 (`recon/compare_b1a_forms.py pbday`): set size, holdings and funders must agree; block count for the month; share of sets unresolved.

### Phase B — event columns, remaining 18 chunks (days 2–8)
`chunks/<chunk>_events.sql`, `--proven` after A2, each left in the warehouse or exported per day 0 step 3. Projected 945. **Gate:** cumulative events ≤ 1,500 by day 8, else C1 or C4.
On landing, per chunk: row count against `universe_p_preevent_by_month` (the monthly P counts already measured), null counts per column, checksum.

### Phase C — heavy tier, remaining 18 chunks (days 9–22, and month 2)
`b1b` and `b2` for all chunks first (about 360 together, cheap, and c06 is H1). Then `b1a` in the chunk order above, about 180 each.
**Day-10 check:** phase B complete and month-to-date ≤ 3,000, else C2.
**Month 1 stops** when the next `b1a` would cross the reserve. On the projection that is after about nine `b1a` chunks.
**Month-2 ruling** uses the measured mean cost of the `b1a` chunks run so far times the chunks remaining; bought only if that is ≤ 3,000.

### Phase D — census and reconciliation (throughout, 200 / 300)
Gate 0 census: one query with `GROUP BY` week over the small tables. The 20-token hand reconciliation (`VALIDATION_1_4_CENSUS.md`, seed 20260922) on day 1: acceptance 19 of 20.

### Phase E — cancel (last day)
Plan cancelled before renewal; any view deleted and storage read back as 0; every export's checksum in the manifest; the Read/Write key revoked if it was used.

### Budget, launch order against projection

| Phase | Launch order (plan / ceiling) | Projection | Comment |
|---|---|---|---|
| A | 400 / 500 (MR-14) | 338 + 60 + up to 50 | whole 2026-08 chunk, plus the two one-day proving runs |
| B | 1,000 / 1,500 | 945 (1,014 with A's share) | fits |
| C | 1,800 / 2,000 + ≤ 3,000 | 3,794 (5,050 at the post-BOOST rate throughout) | fits the 5,000 combined ceiling on the projection; 50 over at the high rate |
| D | 200 / 300 | 30–100 for the census; `b1a` group export a few credits per chunk | the table itself stays in the warehouse on Analyst |
| Month 1 | 3,150 of 3,400 | A 398 + B 945 + D 100 + C about 1,950 | |
| Month 2 | ≤ 3,000 | C remainder about 1,600 | |

---

## 6. Daily ledger entry (`docs/credits.md`), posted to Mando

```
### 2026-MM-DD  (plan day N, phase X)
spent today        ___      (runs: ___, cancelled: ___, exports: ___)
month to date      ___ of 4,000     spendable left above reserve  ___
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
| **C2** | projected heavy remainder > remaining budget | Drop, in this order only: `b3` (already unscheduled), organic-v2 (already unscheduled), `seta_lat_s` (remove the latency columns from `b1a`'s output; saves little, since the funding scan stays), `b2`. Never `b1a`'s set, funding and block columns, never `b1b`. |
| **C3** (the expected path on Analyst) | export rate > 3 per MB | Each chunk query is run as the definition of a materialized view instead of ad hoc: same cost, rows stay on Dune. Analysis passes are ad-hoc queries over the views; only result grids leave. Needs the Read/Write key, expiry before the first cron firing, and the public-table acceptance already given. **`b1a` is never run as a view** (MR-14 ruling 3): its funder groups name wallets, so it is always an ad-hoc query whose rows are exported to `barrel/data/`. It is small once grouped. |
| **C4** | a chunk fails on engine limits or time | Regenerate that month as two halves with `gen_chunks.py`; same budget line. |
| **C5** | an input column is empty, a count does not reconcile, the virtual reserve is not constant | Halt the phase. Null-count across all eras. Patch the generator. Re-run only chunks whose columns are touched. Logged in `credits.md` and the validation document for that column. |
| **C6** | reserve reached with chunks outstanding | Stop. Manifest marks built chunks. H1 evaluated on those, labelled PARTIAL with raw n and block n. |
| **C7** | the runner cancels a query | No re-run of that pattern until it has passed one-day scope again. Second cancellation in the month: phase paused for review. |

## 8. After export (local, no credits)

Inputs: the per-token table, the funder-group rows, the member rows already held for two days and one week. Code: `recon/actors.py` (classifier, collapse, blocks; grouped and member forms give the same record, tested).

* Thresholds for v1.2 are set on the calibration slice (chunks 2025-03 → 2025-07-10) only.
* The burned week 2026-08-10 → 08-16 and the calibration slice are excluded from evaluation; every cell reports raw n and block n.
* H5 and the actor registry use the funder-group rows. One limit to state now: **the registry's actor is a funder seen within four days of a token's first graduation day.** Actors who fund further ahead are unlinked creators until M0b.
* `seta_lat_s` per token is computed from per-group medians. It is exact when the set has one funder group, which is the usual case, and an approximation otherwise.

## 9. Not in this plan

`b3` early-buyer groups, organic-v2, fee-share recipients, collapsed organic counts: built only under the Plus path or a later ruling. Multi-hop funding, live tracking, chatter and H3 are as placed by the launch order's §5.
