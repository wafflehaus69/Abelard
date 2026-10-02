# BARREL M0 — purchase decision (final pre-purchase version)

**Author:** ClaudeCode · **Revised:** 2026-10-01, third version · **For:** Mando, at checkout
**Rule being applied:** `RULINGS_2026-10-01_B.md` (MR-12). **Measurements:** `MATERIALIZATION_UNITS.md`, `HEAVY_TIER_UNITS.md`, `FACTORY_CLASS.md`. **Designs:** `STORAGE_DESIGNS.md`.
**Trial ends:** 2026-10-06 (view-only after). **Balance:** 2,154.9 of 2,500 used; 165.1 spendable above the 180 reserve (as of the end of 2026-10-01).

## The decision, as ruled

1. **If Plus can be bought month-to-month at about $400 or less: buy one Plus month, nothing cut.**
2. **If Plus is yearly-only: buy Analyst month-to-month, two months, with `c07b` and `cluster_*` out.** A third month needs its own ruling.
3. **Nobody presses "Keep Plus".** That is the trial converting at the yearly rate.

The builder's read of the numbers below: path 1 is the better buy if the price condition holds, with one caveat. **25,000 credits cover everything that has been measured; they do not safely cover everything that has been ruled**, because one ruled item has no measurement yet (§3).

---

## 1. What is measured now

| Unit | Measured |
|---|---|
| 60 event columns, one week | 32.4 and 35.0 (two runs) |
| Heavy queries, calibration week (1,573 tokens) | 147.4: aligned set + funding 76.7, early-buyer groups 61.8, concentration 8.8, authorities 0.1 |
| Same queries, one June-2025 day → one post-BOOST day | 89.0 → 125.4 (**×1.41**; ×1.33 without early-buyer groups) |
| What cost follows | days of large table scanned; the per-token model over-predicts by 4× |
| Export, trial (= Plus) rate | about 1 credit per MB |
| Stored size | 368 bytes per token row |

Not measured: organic-v2 in chunk form (one readiness figure, 43.6 per week); `c07b` beyond readiness (11.6 per day scanned); collapsed organic counts (nothing); Analyst's export rate; any pre-BOOST month other than June 2025.

## 2. Credits by phase, 20 monthly chunks

Low = the post-BOOST rate applies only to the post-BOOST chunks. High = it applies to every chunk. Planning uses the high column (MR-10).

| Phase | Low | High | Basis |
|---|---|---|---|
| A. 60 event columns | 1,000 | 1,500 | measured |
| B1. Aligned set, funding, concentration, authorities | 4,300 | 5,400 | measured, both eras |
| B2. Early-buyer groups (`cluster_*`) | 3,250 | 4,600 | measured, both eras |
| B3. Organic-v2 | 900 | 1,800 | readiness figure |
| B4. Token-2022 extensions (c03, c04) | 0 | 100 | none in the measured week |
| C. `c07b` fee-share recipients | 7,000 | 7,000 | readiness figure; post-BOOST rate unknown |
| D. Gate 0 census | 30 | 100 | small tables |
| E. Analysis passes | 0 | 100 | |
| G. Collapsed twins of the organic counts (MR-12 ruling 3) | 3,000 | 4,600 | **not measured.** Assumed about the size of B2: the same funder join over a larger wallet set |

Getting the table out (F) depends on the plan and is in each path.

## 3. Path 1 — one Plus month, nothing cut

25,000 credits, private queries, 15 GB, export at the measured 1 credit per MB.

| | Low | High |
|---|---|---|
| Table and heavy tier (A + B1 + B2 + B3 + B4) | 9,450 | 13,400 |
| `c07b` (C) | 7,000 | 7,000 |
| Census, passes, export-first at 1 credit/MB (D + E + F) | 400 | 900 |
| **Everything measured or priced from readiness** | **16,900** | **21,300** |
| Collapsed organic counts (G), unmeasured | 3,000 | 4,600 |
| **Everything ruled** | **19,900** | **25,900** |

* **The day-15 criterion holds on these numbers:** table and heavy tier are 9,450–13,400 against the 15,000 line.
* **The whole ruled scope is 19,900–25,900 against 25,000.** It fits at the low end and is 900 over at the high end. The Architect's 13,000–18,600 did not include G, which ruling 3 added.
* **Build order that keeps this safe:** A, B1, B3, B2 in the first fifteen days (the criterion). Then `c07b`. Then G, measured on one chunk before it is built, and built only as far as the remaining credits go. If G does not fit, the organic counts ship with their collapsed twins NULL, which reads as unresolved and cannot be mistaken for a count.
* Storage design is export-first: the rate is known, nothing is public, and no view or cron exists.

**Kill criterion for this path (as ruled):** table and heavy tier built by day 15 inside 15,000 credits. If not, stop and re-rule; the month is not extended.

## 4. Path 2 — Analyst, two months, with the cut

4,000 credits a month. `c07b` and `cluster_*` are out. G is out as well, by the same logic: it is unmeasured and does not fit.

| | Low | High |
|---|---|---|
| A + B1 + B3 + B4 | 6,200 | 8,800 |
| Census and passes (D + E) | 30 | 200 |
| Getting the table out (F): in-warehouse 50 · export-first at 3 credits/MB 1,100 · at 5 per MB (five times the rate measured on the trial) 1,900 · at Dune's documented 10 per MB 3,700 | 50 | 1,900 |
| **Total** | **6,300** | **10,900** |

* **Two months (8,000) cover the low end. The high end needs a third,** which the two-month rule sends back for a ruling.
* Storage design follows Analyst's export rate, read at checkout: ≤ 3 credits/MB → export-first; the documented 10 → in-warehouse, with the public table Mando has accepted.
* What this path does not deliver: H2's inputs, `c07b`, and collapsed organic counts. H1, H4, RUG-A and the cost model are complete without them.

**Kill criterion for this path (unchanged):** by day 10, the event table for the full window built and out for 3,000 credits or less, and the first heavy chunk, a post-BOOST month, measured. No third month is bought blind.

## 5. The two paths side by side

| | Plus, one month | Analyst, two months, cut |
|---|---|---|
| Credits available | 25,000 | 8,000 |
| Credits needed (high column) | 21,300 measured scope; 25,900 with G | 10,900 |
| Calendar time | one month | two to three |
| Scope | everything, G as far as credits allow | no `c07b`, no `cluster_*`, no G |
| Queries public | no | yes, under in-warehouse |
| Export rate | known (1 credit/MB) | unread |
| Price | to be read; the rule's ceiling is about $400 | to be read; about $75 a month is the working guess, so $150–225 |

## 6. At the checkout screen

1. Switch "Billed yearly" **off** before reading any price.
2. Read **Plus monthly**. At about $400 or less → path 1.
3. If Plus has no monthly option → path 2. Read **Analyst monthly** and **Analyst's export rate**.
4. Read whether **overage bills automatically** past the allowance. If it does, the runner is given a hard stop at the allowance before the first query.
5. Do not press "Keep Plus".

## 7. Still open after purchase

* G's cost, measured on the first chunk it is run on.
* The fan-out threshold (400 provisional) and the factory rule, both frozen in v1.2 on the calibration slice.
* Organic-v2 in chunk form.
* Columns that older events never filled: `creator`, `is_buy` with the trade amounts, `token_program`. Each has a replacement source; every chunk null-counts its inputs per era (E35).
* M0b (H3) has no storage design on any self-serve plan.

## 8. Amendment, 2026-10-01 — after the chunk list and the burned week

* **The window is 19 calendar chunks, not 20.** With every chunk's query generated (`recon/chunks_manifest.json`), the Analyst core build (event columns, aligned set and funding, concentration, authorities) projects at **4,808 credits**, about 6,200 if every chunk costs the post-BOOST rate. Section 2's B1 and A lines were computed on 20 chunks and are about 5% high.
* **On Analyst, getting the table out is the binding item,** not the build: 1,100 credits at 3 per MB, 1,900 at 5, 3,700 at Dune's documented 10, for the event table alone. Section 4's high total (10,900) uses 1,900; at 10 per MB it would be 12,700. In-warehouse analysis is the expected design on that plan (`RUNBOOK_v1.md` §1 point 3). On Plus it is about 375.
* ~~The burned post-BOOST week agrees with the June 2025 week.~~ **Struck the same day** (`VQR_CHECK.md`): the post-BOOST prices assumed a virtual reserve that a third of pools do not have. The decision-value reasoning in the trial report rests on one pre-BOOST week until the burned week is re-counted.
* **Fees re-measured** after a mapping defect: about 2.2–2.4% round trip since October 2025; post-BOOST buys 99 bps, sells 120.

## 9. Amendment, 2026-10-01 — after MR-14

* **Phase A is 400 / 500** and runs the whole 2026-08 chunk. On Analyst the table is analysed in the warehouse; only the aligned-set funder groups are exported, always.
* **One blocker stands between this document and a purchase:** post-BOOST price columns are halted (`VQR_CHECK.md`). The fix is written and matched the chain on 35 of 35 pools; it needs the Architect's acceptance and one post-BOOST day at a hard cap (30–50 trial credits, requested). Nothing else in `RUNBOOK_v1.md` is open on the builder's side.
* The plan paths, their costs and the checkout steps above are unchanged.

## 10. Amendment, 2026-10-01 — after the review of the MR-14 work

* **Not one blocker but three** (`RUNBOOK_v1.md` §1): post-BOOST prices (B1), the aligned-set query changed after its proving run (B2), and fan-out measured on a window that varies with chunk length (B3). B1 and B2 need up to 120 trial credits; B3 needs a ruling.
* **Organic-v2 is in section 4's Analyst path (line B3, 900–1,800) and has no chunk query.** Either it is scheduled or the Analyst path is 5,300–7,000 without it. To be ruled (`RUNBOOK_v1.md` R3).
* **In-warehouse does not make export free.** Verdicts are applied locally, so the analysis columns still leave Dune: roughly 50–100 MB, 150–1,000 credits depending on the rate (`RUNBOOK_v1.md` R4). Section 4's "in-warehouse 50" is too low.
* The plan paths and the checkout steps are otherwise unchanged.

## 11. Amendment, 2026-10-02 — after MR-15

* **Two of the three blockers are closed, and the third is one run from closed.** Derived reserve: 35 of 35 against pool accounts (10.7 credits). Fan-out: its own query per chunk, proven at one-day scope (0.5 credits). Regenerated aligned-set query: agrees with the earlier rows wherever the definitions are the same (40.6 credits), but a review then found its funder scan stopped at a chunk-level date; it is fixed and needs one more one-day run, after a ruling on whether token-account rent counts as funding (`RUNBOOK_v1.md` §1).
* **Analyst path, as ruled: no organic-v2, no `cluster_*`, no `c07b`.** Core build, 19 chunks: event columns 1,014 · aligned set and funding 3,417 · concentration 367 · authorities 10 · fan-out 579 = **5,387 credits** (about 6,800 if every chunk costs the post-BOOST rate). The aligned-set figure was measured while that query still carried the fan-out scan, so it is on the high side; the fan-out line was priced at 1 credit per day scanned and measured 0.5 on its one day.
* **Plus the one export of the analysis columns** (R4): 50–100 MB, priced on day 0 from the measured rate; phase D's ceiling is that × 1.3.
* **Against two Analyst months (6,800 spendable):** 5,387 + census 100 + export leaves room for the export only if the rate is at or under about 13 credits per MB at 100 MB. At the high build rate (6,800) two months leave nothing for the export, and a third month goes back for a ruling, as already ruled.
* The Plus path is unchanged: everything ruled, including organic-v2, at 19,900–25,900 against 25,000.
* Balance: 2,213.9 of 2,500 used; 106.1 spendable above the 180 reserve.
