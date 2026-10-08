# BARREL M0 — Paid-Month Runbook v1 (operational)

**Author:** ClaudeCode · **Date:** 2026-10-01; last revised 2026-10-07 · **Returns:** the Architect's launch order (`RUNBOOK_ARCHITECT_2026-10-01.md`, MR-13) with the operational detail filled in.
**Status: READY TO PURCHASE, with the aligned-set query's proving run as step one of phase A (§1).** Revised after MR-14, MR-15, MR-16 and MR-17 (`RULINGS_2026-10-07.md`) and three adversarial reviews. The third, of the aligned-set query as MR-16 changed it, found three minor points; two are fixed and one is a limit of the rule as ruled (`B1A_GROUPED_VALIDATION.md`, last section). The trial closed on 2026-10-06.
**Plan:** by MR-16, one Plus month if it is sold month-to-month under $400 (§5b); otherwise Analyst, month-to-month, 4,000 credits a month, 3,400 spendable after the 15% reserve (§5), where two months no longer fit the core build with margin and a third needs its own ruling.

---

## 1. Status before purchase

**Ready to purchase. Day 0 can be any day from 2026-10-06.** Nothing the Architect ordered before purchase is outstanding. One thing was supposed to be proven on the trial and was not; by ruling it is step one of phase A, and it is listed first below so that nobody mistakes "ready" for "everything proven".

### What is not proven, and where it is proven instead

| | State | Proven at |
|---|---|---|
| **Aligned-set query (`b1a`), the most expensive query of the build** | The text on disk (per-token windows, slot cutoff, tie-break, lengthened funder scan, MR-16 rent rule and link type) **has not run.** Its trial re-run was cancelled at the ruled cap of 70 after 243 seconds, billed 73.7, no result. The text before the last two changes ran on 2026-10-02 (40.6 credits) and agreed with the earlier member rows wherever definitions were unchanged. | **Phase A step A1, cap 150.** Before any chunk. |
| Its per-chunk cost | Projected at about 180 pre-BOOST and 245 for 2026-08, on an earlier text. The cancelled run shows the lengthened funder scan costs more than its share; a one-day run pays 15 days of that scan for one day of tokens, a chunk pays 45 for 30, so the projection is a lower bound, not an estimate. | Phase A step A4 |
| `tx_id` and `tx_index` filled in every era on both transfer tables | Exist on both (zero-row probe). The null-count was cancelled at its cap. If `tx_id` is empty on either table the rent rule excludes nothing and every creator tie reads `sol_funding` (the predicate was made null-safe after the third review; before that an empty `tx_id` on the SOL table would have removed funders). | Day 0 step 5 (`tx_id_nullcount_probe.sql`, three eras, cap 30); and the link column of A1 (about 300 delivery ties expected on 2026-09-01) |
| Column 86 (`seta_lat_s`) | Compared only against rows that used an earlier definition | A1: member form and grouped form of one text, compared |
| Fan-out query at month scope | One-day scope only (0.5 credits) | Phase A step A1b |
| Any chunk longer than a day for `b1a` | none | Phase A step A4 |

### What MR-16 settled (`RULINGS_2026-10-05.md`)

| Item | Result |
|---|---|
| Rent is not funding | Implemented: a transfer inside the member's own first-acquisition transaction never chooses its funder |
| Creator tie typed | `sol_funding` / `token_delivery_by_creator` / `both`, on every member row and in the group key |
| Blocks as connected components | Ratified; `actors.assign_blocks` |
| Push main | Done. The two doctrine entries are **E37 and E38** on main, because main had taken E34–E36 meanwhile; BARREL's earlier documents say E34 and E35 and mean these. |
| R7 | Closed: the off-machine backup of `barrel/data/` is Mando's |
| Plan | One Plus month if monthly and under $400; §5b |

### What MR-17 settled (`RULINGS_2026-10-07.md`)

| Item | Result |
|---|---|
| Phase A ceiling | **800 on Analyst (from C), 900 on Plus.** A1 at 150 per form stays the first query; if it cannot finish inside 150 the plan goes back to design before a chunk runs. |
| Wallets the creator delivered tokens to | **They are the creator's actor, whoever paid their SOL.** `actors.DELIVERY_AS_CREATOR = True`, the default of both readers. Because actors are components, a delivered wallet's own purpose-built funder, and the other wallets that funder paid, join the creator with it (the builder's reading, recorded in the ruling file). |
| Scope of the rent rule | **By transaction, until A1 reports how many groups it touches.** The count comes from `recon/rent_rule_report.py`, run in step A1. From rows already held, graduations of 2026-09-01: **bundle groups not paid by the creator, no funder differs for any of 38 members (9 groups, 8 tokens)**; creator's side, 342 of 76,961 on a net count that is a floor, 301 of them the creator's own payments. Blind spots and limits: `RULINGS_2026-10-07.md`. |
| Drop order on Plus | **Collapsed organic counts, then early-buyer groups (`b3`), then organic-v2, fee-share recipients last.** H1's inputs are the last thing to go. §5b. |

### For the Architect, not blocking

* **Build order after the core, and what "heavy tier" names in the day-15 criterion: one decision, best before day 2 and by about day 6 at the latest.** The criterion is "table and heavy tier built by day 15 inside 15,000 credits". The builder proposes the reverse of the ruled drop order (fee-share first), so that a short month drops what was ruled and not what was scheduled last. On that order the criterion has to read "core plus fee-share" (12,565–14,170, met if fee-share's last chunk lands by day 15); the reading in `PURCHASE_DECISION.md` §3 (core plus organic-v2 and early-buyer groups, 9,715–13,570) cannot be met on it, because those two are then finished at 16,715–20,570 after day 15. On §3's own order that reading is met and fee-share runs last, where a short month drops it. **Until ruled, no chunk of fee-share, organic-v2 or early-buyer groups runs.** Days 1 to 5 spend the same credits either way, but not the same writing: §5b writes fee-share first (proven by day 5, organic-v2 by day 8, `b3` by day 9), and §3's order needs the other two first. Ruled on day 6, §3's order would leave its two groups six or seven chunk days before day 15; an answer before day 2 keeps both orders fully open. Writing the three queries before purchase, at zero credits, would leave only their one-day proofs for days 2 to 5 and keep both orders open until day 6; that work is not ordered. The ruled criterion is not relaxed in the meantime.
* **Blocks do not see delivery.** A delivered wallet is the creator's actor in the actor count, but the block for effective-n joins creators to their linking funders only, and the build form does not name delivered wallets. A delivered wallet that is itself the creator of another token therefore stays in its own block. A1's member form names them for one day; the count is taken there and reported.
* **The evidence on the rent rule is one post-BOOST day and a net count.** Bundle groups are thin on it (38 members). The pre-BOOST rows held carry more (571 bundle members in 41 tokens over the June 2025 week) but both June results predate the slot rule, so no difference can be taken there, and A1 is the same post-BOOST day. Group rows do not name members, so the count is net per token: exact for a bundle group with one funder, a floor elsewhere. An exact count, or a pre-BOOST one, needs a column in the query or one more one-day run without the exclusion. Neither is ordered.
* **Analyst only: phase C's month-1 ceiling of 1,450 now binds before the reserve** (§5). Month 1 then ends 235–385 credits short of the reserve. Whether phase A's unspent ceiling returns to C is asked only if the plan is Analyst.
* **Under Plus, three column groups have no runnable chunk query yet** (§5b): fee-share recipients, organic-v2, and the early-buyer rewrite. On the proposed order fee-share is written first; on §3's order, organic-v2 and the early-buyer rewrite. The work is two to four days inside the month.
* **Against the 15% reserve the uncut Plus scope is tight** (§5b): 17,145–21,370 before the unmeasured collapsed organic counts, against 21,250 spendable. The ruled drop order governs.

### What MR-15 closed (`RULINGS_2026-10-02.md`), kept for the record

| Item | Result |
|---|---|
| **B1** derived virtual reserve | **Closed.** One post-BOOST day (2026-08-10), 10.7 credits. Derived reserve matches the pool account on 35 of 35 seeded pools (largest difference 46 lamports), and on the one pool carrying a third value (19.87 SOL). Halt lifted. `VQR_CHECK.md` |
| Burned-week price paths, re-counted locally | Done for the one day run; the other six days when phase A builds 2026-08. `PRICEPATH_BURNED_WEEK.md` |
| **B2** aligned-set query with the ratified definitions | Ran once (40.6 credits): 1,087 of 1,087 tokens agree on size, holdings, flows and supply. **The text has changed twice since (funder scan, rent rule) and has not run: first row of the table above.** `B1A_GROUPED_VALIDATION.md` |
| **B3** fan-out on a fixed window | **Closed.** One query per chunk over the full calendar month, recipients per day; funders substituted at run time. Proven at one-day scope, 0.5 credits. `recon/gen_fanout.py`, `actors.fan_rates` |
| **R1** member-funder is one actor | Implemented in `recon/actors.py`, both forms, tested |
| **R2** block keyed by address | Component over the whole set; ratified by MR-16 |
| **R3** organic-v2 cut on Analyst | H5 re-registered without it (`M0_TASKING.md` MR-15). Under Plus it is built (§5b). |
| **R4** analysis-column export | Phase D ceiling = day-0 rate x size x 1.3 (§5) |
| **R5** view mode | Built only if day 0 selects in-warehouse (Analyst), before phase B. Half a day. Not built. Not needed on Plus, which is export-first. |
| **R6, R8, R9** | Recorded |
| **R7** | Closed by MR-16 |

### Rulings applied (MR-14)

| # | Ruling | Where |
|---|---|---|
| 1, 8 | 19 chunks; window end 2026-09-20 signed | §4 |
| 2 | Phase A 400 / 500 from C; runs the whole 2026-08 chunk including `b1a` | §5 |
| 3 | In-warehouse expected on Analyst; day 0 measures the rate; `b1a` output always exported, never materialized | §5, §7 C3 |
| 4 | Sampled window as proposed | §7 C1 |
| 5 | Definitions may live in a query, verdicts may not; the bundle's five is applied locally; no request with a registered constant reaches Dune | §2 |
| 6 | Rows to gitignored `barrel/data/`; manifest committed the same day | §2 (`--export`) |
| 7 | Grouped `b1a` one-day run | done (236 of 236 tokens), and again under MR-15 B2; the text has changed since (§1, first row) |

---

## 2. Standing rules, and what enforces each

| Rule | Enforced by |
|---|---|
| Expected cost is the hard cap for an unproven pattern | `recon/dune_run_sql.py`: `--expect` is the cap unless `--proven`; above 25 needs `--confirm` |
| New patterns at one-day scope first | every generator writes a one-day file; §4's table is the record of what has run |
| Null-count every input column per era | `recon/sql/*_probe.sql`; day 0 step 5 |
| Cost runs do not fetch rows | `--no-rows`. The rows of a run already made are read with `--refetch <execution id>`: nothing is executed again. It takes only an execution this runner made for the same file, and only a completed one. Also the answer to a fetch that came back incomplete. |
| Rows that name wallets stay out of the tracked tree | a query whose header says its rows name wallets is forced to `--private-rows` whatever flags were given; owner wallets are dropped from any fetched row |
| Reserve: 15% of the month's allowance | the runner computes it on the allowance the usage API reports (600 of 4,000) and refuses a run that would cross it. The trial keeps its ruled 180. |
| Daily ledger | `docs/credits.md`, one block per day (§6) |
| No verdict threshold in any query, saved or not | `recon/verdict_constants.py`, called from `dune_roundtrip.dune`, the one function every request passes through (ad hoc, saved query, view definition), and from the runner and the chunk review. No override. A newly ruled threshold is registered in the commit that records the ruling. It is a tripwire: it cannot see a verdict nobody registered, so queries are still read. |
| No bare `SELECT *` | same function: a column probe uses `LIMIT 0` and reads the column names, so no row of metadata or wallets is fetched |
| Exports | `--export`: rows to gitignored `barrel/data/`; SHA-256, row count and per-column null count appended to `recon/export_manifest.json`, which is committed. `b1a` groups are always exported and never materialized. |
| Owner wallets never in a saved query | `__NOT_OWNER(col)__` is substituted at run time from `barrel/private/`; the chunk review fails any wallet-bearing query without it |
| In-flight counter is not a meter | It jumps by up to 56 credits and has read above the final bill. Cancellation bounds slow overruns only; the one-day rule is the protection. |
| A running query is never left without a cap | The runner watches until the execution finishes or is cancelled; a cancellation is read back and repeated if it did not take; on lost contact it tries to cancel and prints the id. `--max-seconds` (default 420) is set per run: 900 for A1, 1,500 for chunk files until A4 has measured their time. Tested with Dune stubbed (`tests/test_runner_watchdog.py`). |

## 3. Pre-purchase checklist

| Item | State |
|---|---|
| Post-BOOST price-path week, burned, by seed, recorded first | **Run (44.05 of 50); its price results are STRUCK** (`VQR_CHECK.md`). Re-counted from per-token rows after B1. Week 2026-08-10 → 08-16 stays burned. |
| `PURCHASE_DECISION.md` with both paths | Done (third version, amended through §13: §12 carries the MR-16 plan rule and what the Plus path still needs built; §13 the MR-17 drop order, the proposed build order and phase A's ceiling). |
| Chunk list, queries generated and reviewed, zero credits | 19 chunks; **95 scheduled files (five per chunk), review problems 0**; 19 `b3` files generated, not scheduled, refused by the runner as written. `recon/chunks_manifest.json`. |
| `RUNBOOK_v1.md` | this document |
| Defects found on the way | `ix_name` NULL on every buy event; virtual reserve not constant; Dune's mayhem flag empty before late August 2026; `b1a` definitions that varied with chunk length; holes in the first threshold check. |

Trial closed 2026-10-06: **2,300.2 of 2,500 credits used.** About 1,010 produced data still in use; about 1,290 was lost to cancellations, a runaway, void runs and oversized fetches (`TRIAL_BURNDOWN_ORDERS.md`).

---

## 4. Chunk list

One calendar month of stratum-P graduations per chunk. Files: `recon/sql/chunks/<chunk>_<query>.sql`.

| Query | Feeds | What has actually run |
|---|---|---|
| `events` | the event-derived columns | **The text on disk ran on one post-BOOST day (2026-08-10), 10.7 credits; reserve 35 of 35 against pool accounts.** Earlier texts: one day and two weeks pre-BOOST. |
| `b1a` | aligned set grouped by funder and creator tie: c07, c08, `seta_*`, `seta_lat_s`, registry | An earlier text (per-token windows, slot cutoff, tie-break) ran on one post-BOOST day, 40.6 credits. **The text on disk adds the lengthened funder scan, one ledger day, the rent rule and the link type. Its one trial run was cancelled at the cap without a result. Not run.** |
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
5. **Null-count probes, about 10 credits, plus `tx_id_nullcount_probe.sql` (cap 30; it cost 12.5 for two eras on the trial and now covers three):** one hour per era for every input column of `events`, `b1a`, `b1b`, `b2`; and `recon/sql/buy_field_swap_by_day.sql` (MR-14 order 4): per day from 2026-08-08 to 09-02, how many buys have each quote field the larger, on stratum-P pools and on all pools, to explain why the burned week and the single sampled days disagree.

### Phase A — proving run, then scaling check (days 1–2). Ceiling 800 on Analyst, taken from phase C; 900 on Plus (MR-17 ruling 1).
| Step | File | Cap |
|---|---|---|
| **A1** | **First query of the month.** `heavy_b1a_grouped_pbday.sql`, one post-BOOST day (2026-09-01), run once with `--export --max-seconds 900`; a failed fetch is repeated with `--refetch`, never by running again. Compared with the member rows on disk. Then `heavy_b1a_members_pbday.sql` for the same day, for column 86 and the link type. | **150 each, hard.** The trial run of the grouped text was cancelled at 72 while still running. If the whole of the earlier 40.6 had scaled with the funder scan (6 days to 15) it would be about 102; 150 leaves half as much again. |
| A1b | `chunks/2026-08_fan.sql` with the chunk's funders, after A4 | 40, hard |
| A2 | `chunks/2026-08_events.sql` | 90, hard |
| A3 | `chunks/2026-08_b1b.sql`, `2026-08_b2.sql` | 35 and 5 |
| A4 | `chunks/2026-08_b1a.sql` | 320, hard |

**Gate:** measured cost of the 2026-08 chunk ≤ 1.3 × 338 = **440**. Above that → C1. The gate is on the chunk; A1 is outside it. The caps of every step add to 790 (A1 300, fan-out 40, events 90, concentration and authorities 40, aligned set 320), inside the 800 ceiling.
**If A1 is cancelled at its cap: stop. No chunk runs. The whole plan goes back to design before a chunk runs** (MR-17 ruling 1), at zero credits: the query first (the funder scan is the suspect; it can be split from the member scan into its own query, the way fan-out was), and with it §4's projection and the phase budgets computed on this text. The Architect is told the same day. C7 applies to the pattern; a redesigned query passing one-day scope does not by itself release the chunks.
**Also checked here:** the link column of A1 (about 300 `token_delivery_by_creator` ties expected on 2026-09-01; none at all means `tx_id` is empty and the rent rule did nothing: C5). **the count MR-17 ruling 3 waits for:** `python barrel/recon/rent_rule_report.py pbday` differences A1's export against the rows of 2026-10-02 and reports, for bundle groups and for the creator's side apart, how many groups and members differ, net per token, a zero printed as a zero: on the bundle side every difference is the transaction rule; on the creator's side the 8 members that first acquire on or after 2026-09-03 also carry the longer funder scan (from rows already held: no funder differs for the 38 bundle members; 342 net on the creator's side, about 300 of them the creator's rent, which is where those payments show). `python barrel/recon/compare_b1a_forms.py pbday`, run once after the grouped form against the member rows on disk and again after the member form: it reports the share of sets unresolved with delivered wallets as the creator's actor (ruled) and under the reading before the ruling, and from the member form how many delivered wallets are themselves the creator of another token that day (blocks do not see delivery, §1) and how many creator-paid wallets at or above the bundle cut have no funder. Against the member rows of 2026-10-01 these differences are expected and are not defects: the latency median on several hundred tokens, because those rows start the clock at the funder's last transfer and the text on disk at its first (667 of 1,087 in the comparison already held; column 86 is checked by the second run, against A1's own member form); a different funder where the text on disk orders same-second senders by slot (the 39 members that changed sender in the comparison already held) or counts a separate transaction in the second of the first action; the 8 members that first acquire on or after 2026-09-03, which the longer scan now reaches; the creator-funded count one lower on about 6 of 1,087 tokens (creator self-transfer guard); and the link column, which those rows lack, so against them both forms are read as before MR-17. Actors after collapse can differ only where the funders do, and are not comparable where A1 names a funder the earlier rows carry no fan measure for (241 tokens in the comparison already held). The rent payments do not show there: the rows of 2026-10-01 never counted them. Each run also writes its own file (`recon/out/b1a_forms_agreement_pbday_<member rows>_<grouped rows>.json`), so the second does not replace the first. Also checked here: block count for the month; share of sets unresolved; the derived reserve against pool accounts (day 0 step 4).

### Phase B — event columns, remaining 18 chunks (days 2–8). Budget 1,000 / 1,500.
`chunks/<chunk>_events.sql`, `--proven` after A2. Projected 945. **Gate:** cumulative events ≤ 1,500 by day 8, else C1 or C4.
On landing, per chunk: row count against the monthly P counts already measured (`VALIDATION_1_2_GRADUATIONS.md`), null counts per column, and the share of pools with no derived reserve.
**In-warehouse needs R5 built first.**

### Phase C — heavy tier, remaining 18 chunks (days 9–22, and month 2). Budget 1,250 / 1,450 in month 1 (550 moved to A in all: 250 by MR-14 ruling 2, 300 by MR-17 ruling 1; taking the 300 off both figures is the builder's arithmetic), up to 3,000 in month 2.
`b1b` and `b2` for all chunks first (about 350 together, cheap, and c06 is H1). Then `b1a` in the chunk order above, about 180 each, always `--export`, each followed by its `fan` query (`--funders-from` that chunk's export; about 30 each, 579 in all, inside this phase's budget as ruled).
**Day-10 check:** phase B complete and month-to-date ≤ 3,000, else C2.
**Month 1 stops** when the next `b1a` (with its `fan`) would cross phase C's month-1 ceiling of 1,450 or the reserve, whichever comes first. On the projection the ceiling comes first, after about five `b1a` chunks (350 for `b1b` and `b2`, then about 210 a chunk with its fan-out).
**Month-2 ruling** uses the measured mean cost of the `b1a` chunks run so far times the chunks remaining; bought only if that is ≤ 3,000.

### Phase D — census and reconciliation (throughout). Budget 200 / 300.
Gate 0 census: one query with `GROUP BY` week over the small tables. The 20-token hand reconciliation (`VALIDATION_1_4_CENSUS.md`, seed 20260922) on day 1: acceptance 19 of 20. **The analysis-column export (MR-15 R4) is in this phase: one column-pruned export, and the phase's ceiling becomes the day-0 measured rate x the export's size x 1.3.**

### Phase E — cancel (last day)
Plan cancelled before renewal. **Before any view is deleted:** every column the local analysis needs is exported and its manifest entry committed. Then views deleted, storage read back as 0, the Read/Write key revoked if it was used.

### 5b. If the plan is Plus (25,000 credits, one month; 21,250 spendable after the 15% reserve)

The launch order says the same phases run uncut and the gates do not move. Line items are those of `PURCHASE_DECISION.md` §2–§3 with the core replaced by the 19-chunk projection of §4. Low = the post-BOOST rate on post-BOOST chunks only; high = on every chunk. **The aligned-set line inside C is a lower bound until A1 and A4 have run.** Phase A's ceiling is ruled (MR-17); the other ceilings are projection x 1.3 and are a proposal.

**Rows are in the build order the builder proposes: the reverse of the ruled drop order.** MR-17 ruling 4 rules the drop order; the build order is the builder's consequence and is to be confirmed together with the day-15 reading (§1). The point of it: what is dropped when the month runs short is then what has not been started, not what happened to be scheduled last. **No chunk of P3, P1 or P2 runs before that is ruled.**

| Phase | Days | Low | High | Cumulative | Ceiling | Content |
|---|---|---|---|---|---|---|
| A | 1–2 | 520 | 670 | | **900, ruled** | A1 (two forms, cap 150 each), then the whole 2026-08 chunk and its fan-out query. Gate as in phase A above. |
| B | 2–6 | 945 | 1,300 | | 1,700 | event columns, 18 chunks |
| C | 3–12 | 4,100 | 5,200 | **5,565–7,170** (core) | 6,800 | concentration and authorities for all chunks; `b1a` and `fan` for 18 chunks |
| P3 fee-share recipients (`c07b`) — **dropped last** | written and proven 2–5; chunks 6–15 | 7,000 | 7,000 | 12,565–14,170 | 9,100 | **chunk query to be written**, local decode to be wired; one-day proof first. Feeds the aligned set and RUG-A (H1). Post-BOOST rate unknown. |
| P1 organic-v2 — dropped third | written and proven 5–8; chunks after day 15 unless month-to-date stays under 15,000 | 900 | 1,800 | 13,465–15,970 | 2,300 | **query to be written**; one-day proof first. Feeds H5 (exploratory). |
| P2 early-buyer groups (`b3`) — dropped second | rewritten and proven 6–9; chunks 16–24 | 3,250 | 4,600 | 16,715–20,570 | 6,000 | **rewrite needed** (returns counts and a price; the group-size cut is applied locally); one-day proof first. Feeds H2. |
| D | throughout | 430 | 800 | **17,145–21,370** | 1,100 | census, and export-first at the Plus rate (measured 1 credit per MB on the trial; re-measured on day 0); nothing public |
| P4 collapsed organic counts — **dropped first** | after P2 | 3,000 | 4,600 | 20,145–25,970 | — | **unmeasured**, priced by analogy. Measured on one chunk first; built only as far as credits above the reserve allow. Unbuilt twins ship NULL, which reads as unresolved. |
| E | last day | 0 | 0 | | | cancel before renewal; nobody presses "Keep Plus" |

* **Day-15 criterion (ruled): "table and heavy tier built by day 15 inside 15,000 credits"; if not, stop and re-rule; the month is not extended.** Which column groups "heavy tier" names is not settled, and it is tied to the build order (§1). On the order of this table it has to mean the core plus fee-share: 12,565–14,170, met only if fee-share's last chunk lands by day 15. The earlier decomposition (core plus organic-v2 and `b3`, 9,715–13,570 taken alone) cannot be met on this order: those two are finished at 16,715–20,570, after day 15. Whatever is ruled, by day 15 the core (A + B + C) must be built and month-to-date at or under 15,000; that is the part both readings share, it is weaker than either, and it is not offered as a substitute for the ruling.
* **Against the reserve line the scope is tighter than against the allowance.** Without P4: 4,105 to spare at the low column, **120 over at the high column.** With P4: 20,145–25,970.
* **Drop order, ruled (MR-17): P4 collapsed organic counts, then P2 early-buyer groups, then P1 organic-v2, and P3 fee-share recipients last.** The core is never dropped; inside it C2's order stands.
* **The schedule risk is the writing, not the credits:** none of P3, P1 and P2 has a runnable chunk query today. On the proposed order fee-share is written and proven first, in days 2–5, while B and C run; on §3's order organic-v2 and the early-buyer rewrite come first.
* Exports under Plus go to gitignored `barrel/data/` with the committed manifest; **`barrel/data/` is backed up off this machine by Mando** (MR-15 R7).

### Budget, launch order against projection

| Phase | Plan / ceiling after MR-14 and MR-17 | Projection | Comment |
|---|---|---|---|
| A | ceiling 800 (MR-17) | 338 for the chunk, 30 for its fan-out, and A1: at least 150 for the two forms, up to 300 | 520–670 on the projection; fits |
| B | 1,000 / 1,500 | 945 for 18 chunks | fits |
| C | 1,250 / 1,450 (MR-17: 300 to A; taking it off both figures is the builder's arithmetic) + ≤ 3,000 | 3,525 for 18 chunks plus 579 for fan-out = 4,104; about 5,200 at the post-BOOST rate throughout | fits the 4,450 combined ceiling on the projection; **over it by about 750 at the high rate**, which the day-10 check and the month-2 ruling govern |
| D | census 200 / 300; export ceiling = measured rate x size x 1.3 (MR-15 R4) | 30–100 for the census; `b1a` and `fan` exports a few credits per chunk; analysis columns 50–100 MB | |
| Month 1 | 3,150 plan (unchanged: the 300 moves from C to A), 3,400 spendable | A 520–670, B 945, D 100, C to its ceiling of 1,450 | C stops at its ceiling, 235–385 short of the reserve; running C to the reserve is over its ceiling and needs a ruling (§1) |
| Month 2 | ≤ 3,000 | C remainder about 2,650 with fan-out | |

---

## 6. Daily ledger entry (`docs/credits.md`), posted to Mando

```
### 2026-MM-DD  (plan day N, phase X)
spent today        ___      (runs: ___, cancelled: ___, exports: ___)
month to date      ___ of 4,000     spendable left above the 600 reserve  ___     (Plus: of 25,000, reserve 3,750)
phase to date      ___ of plan ___ / ceiling ___
chunks landed      events __/19   b1b __/19   b2 __/19   b1a __/19
measured per chunk events ___   b1a ___   (projection 53 / 180)
projection to end  ___      month 2 needed: yes / no / not yet known
gates              A1 ___   A ___   day-8 ___   day-10 ___     (Plus: A1, A, day-15 at 15,000)
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
* **A wallet the creator delivered tokens to is the creator's actor, whoever paid its SOL** (MR-17 ruling 2; `actors.DELIVERY_AS_CREATOR`). A set is unresolved only when a member that was not delivered has no funder found. The reading before the ruling stays computable for comparison.
* Actor counts use R1; blocks are assigned over the whole evaluation set (`actors.assign_blocks`); both read the fan-out rate table built by `actors.fan_rates` from each chunk's fan-out export. The line between hub and purpose-built is a per-day rate set in v1.2; until then no classification is final.
* **H5 on the Analyst path is registered without organic-v2** (MR-15 R3).

## 9. Not in this plan

On Analyst: `b3` early-buyer groups, organic-v2, fee-share recipients and collapsed organic counts are not built. On Plus they are §5b's P1–P4, each written and proven at one-day scope inside the month before it is scheduled. Multi-hop funding, live tracking, chatter and H3 are as placed by the launch order's §5.

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
