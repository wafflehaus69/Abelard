# BARREL M0 — purchase decision (burn-down item 5)

**Author:** ClaudeCode · **Date:** 2026-09-30 · **For:** Architect review, Mando's decision
**Trial ends:** 2026-10-06 (account becomes view-only). **Trial balance:** 1,500.7 of 2,500 used; 819 spendable above the 180 reserve.

## Recommendation, in one line

**Buy Analyst, month-to-month, with a scope cut:** fee-share recipients (`c07b`) leave M0, and the heavy gate columns are bought only after a day-10 measurement.

---

## 1. What was measured (item 3, `MATERIALIZATION_UNITS.md`)

| Unit | Measured |
|---|---|
| Build, 60 event-derived columns, one week of P (1,573 tokens) | 32.4 credits |
| Same query, one graduation day (241 tokens) | 27.4 credits |
| Refresh | 33.3 credits (a full rebuild) |
| Stored size | 368 bytes per token row |
| Full-table read | 0.028 credits |
| Export | about 1 credit per MB of JSON returned (3.0 credits for the week's 3.25 MB) |
| Per-swap rows, one day | 4.27M rows, 480 MB, 7.5 credits to build |

Three facts follow. Build cost follows the days scanned, not the tokens. Storage is a non-issue for the per-token table (about 66–130 MB of Analyst's 1 GB) and a hard wall for per-swap rows on every plan. Export is roughly 40× cheaper than the earlier budget assumed.

## 2. Projected credits for M0 without H3, by phase

Low and high are both stated because the builder's estimates in this project have run over three times (840, 85 and 85 credits against expectations of 8, 25 and 25). Planning should use the high column.

| Phase | Low | High | Basis |
|---|---|---|---|
| **A. Materialize P, 60 event-derived columns** | 400 | 1,000 | Low: one pass over the window (two-point fit, and it may exceed the engine's limits). High: 19 monthly chunks. |
| **B. Heavy columns** (c01–c06, c07, c08, `seta_*`, `cluster_*`, org2) | 2,500 | 7,000 | Low: readiness scan units. High: today's one-day run of the funding + ledger + bonding-curve join cost 85 credits, which scales to about 7,000 if it is as scan-bound as the event query. **The least certain line here.** |
| **C. `c07b` fee-share recipients** | 7,000 | 7,000 | 11.6 credits per day of raw fee-program calls. **Cut from M0.** |
| **D. Gate 0 census** | 30 | 100 | Counts over the small create / complete / pool tables. |
| **E. H1, H2, H4 passes** | 0 | 0 | Computed locally from the exported table. Verdicts and thresholds never go into Dune anyway (order 6). |
| **F. Export of the table** | 375 | 1,900 | Low: Analyst bills like the trial. High: the documented 5× Analyst rate. Exporting only the needed columns cuts it proportionally. |
| **Total without `c07b`** | **3,300** | **10,000** | |
| Total with `c07b` | 10,300 | 17,000 | |

**Months of Analyst (4,000 credits each): one at the low end, three at the high end.** The spread is almost entirely phase B.

## 3. The scope cut, and why it is staged

Phases A, D and F together are **800–3,000 credits and fit inside the first month on any reading.** That table alone answers the second of M0's three questions (naive expectancy at each entry lag, net of real fees), plus the bot-layer markup, reserve decay, and the cost model per era. It does **not** answer H1 (the gate) or produce the RUG-A label, which need phase B.

So month one buys A + D + F, and in its first ten days also measures phase B's unit on **one calendar month of the window**, built as a chunk. That replaces the 2,500–7,000 range with a number before any second month is paid for.

`c07b` is cut because its 7,000 credits exceed everything else combined, the column is designed to be NULL-able, and the aligned set works without it (creator, funded wallets, bundle wallets).

## 4. Kill criterion, restated against these numbers

By **day 10** of the first Analyst month:

1. **The event-derived table for the full P window is built and exported for 3,000 credits or less.** If not: no renewal, and M0 is re-scoped.
2. **Phase B's unit is measured on one month of the window.** If it projects above **8,000 credits** for the whole window (two further Analyst months), the gate is run on the pre-defined sampled window instead (one contiguous 90-day block per era, chosen before any result is seen, every cell labelled SAMPLED), and no third month is bought blindly.

## 5. Analyst versus Plus, and the public table

Plus is $349 a month billed yearly, with 25,000 credits, 15 GB, private queries and export at a fifth of Analyst's rate. One Plus month would cover everything in the table above, `c07b` included. Analyst at month-to-month pricing is about $65–75 a month, so two months is roughly $130–150 and three is $195–225. **Analyst is cheaper unless phase B lands at the top of its range**, which is exactly what the day-10 measurement decides: if it projects above 8,000 credits, a single Plus month costs less than three further Analyst months, and that becomes the comparison to rule on. What Plus adds beyond credits is not needed: per-swap storage does not fit on Plus either (a 60-day slice is 27–81 GB), and Mando has accepted the public table. The public table's mitigations are in place: neutral column names with the mapping kept only in the repo, no owner wallet in any saved query, and no verdict, threshold or hypothesis logic ever saved to Dune.

## 6. What must be read at checkout before paying

1. **The monthly price with "Billed yearly" switched off.** The $65 shown is the yearly-billed rate, a $780 commitment that defeats the kill criterion.
2. **Whether Analyst bills overage past 4,000 credits automatically.** If it does, the runner gets a hard stop at the allowance.
3. **Analyst's export rate**, if shown. It moves phase F between 375 and 1,900.
4. Do not press "Keep Plus".

## 7. What is not settled

* **Phase B's cost**, the dominant uncertainty. It is deliberately left to a paid measurement on one month of data, with a pre-defined fallback.
* **Whether one pass over the whole window runs at all** on Analyst's engine. If not, phase A costs the high figure.
* **Item 4 (calibration distributions) did not complete on the trial.** 4a's one-day run cost 85 credits and returned nothing, because the deployer column is empty in 2025 create events; the query is fixed and not rerun, pending a ruling. 4b was not started. Neither affects the purchase arithmetic: they are distributions for v1.2, not cost units.
* **M0b (H3)** has no workable storage design on any self-serve plan and needs one before it is costed.
