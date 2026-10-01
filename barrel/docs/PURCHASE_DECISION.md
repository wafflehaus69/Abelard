# BARREL M0 — purchase decision, revised with the heavy tier measured

**Author:** ClaudeCode · **Revised:** 2026-10-01 (first issued 2026-09-30) · **For:** Architect review, Mando's decision
**Orders:** `RULINGS_2026-10-01.md` order 6 (MR-10). **Measurements:** `MATERIALIZATION_UNITS.md`, `HEAVY_TIER_UNITS.md`. **Designs:** `STORAGE_DESIGNS.md`.
**Trial ends:** 2026-10-06 (view-only after). **Balance:** 1,842.6 of 2,500 used; **477.4 spendable** above the 180 reserve.

## One line

**Buy with a cut: Analyst, month-to-month, two months, with `c07b` and the four `cluster_*` columns out of the first build.** The ratified kill criterion's comparison (a single Plus month) is triggered by the measured heavy tier and should be priced at checkout before paying.

---

## 1. What changed since the first version

| | 2026-09-30 | Now |
|---|---|---|
| Heavy columns, full window | 2,500–7,000, resting on a run that returned nothing | **7,100 measured** (monthly chunks, June-2025 volume), plus organic-v2 at 900–1,800 unmeasured in chunk form |
| The 85-credit aligned-set query | "scales to about 7,000" | 14.65 after rewriting it to scan each table once |
| What cost follows | days scanned (event columns only) | **days scanned, confirmed on the heavy tier**: the per-token model over-predicts the week by 4× |
| Largest single line in the heavy tier | unknown | the aligned-set funding query (178 per monthly chunk), then the early-buyer groups (151) |
| Era coverage | one era | **still one era** (June 2025). The open risk. |

## 2. Projected credits for M0 without H3

Monthly chunks, 20 of them. "Plan on the high column" is a project rule (MR-10).

| Phase | Low | High | Basis |
|---|---|---|---|
| **A. 60 event-derived columns** | 1,000 | 1,500 | Measured twice on the week (32.4, 35.0). High allows for post-BOOST volume. |
| **B1. Aligned set, funding, concentration, authorities** (c01, c02, c06, c07, c08, `seta_*`, `seta_lat_s`, `blk`) | 4,050 | 6,100 | Measured, day and week. 200 per chunk. High = ×1.5 for the unmeasured era. |
| **B2. Early-buyer groups** (`cluster_*`, H2 only) | 3,050 | 4,600 | Measured, day and week. 151 per chunk. **Proposed cut.** |
| **B3. Organic-v2** (`org2_*`) | 900 | 1,800 | Readiness: 43.6 for one post-BOOST week. Not re-measured in chunk form. |
| **B4. c03, c04** (Token-2022 extensions) | 0 | 100 | None in the measured week. Late-window only. |
| **C. `c07b`** | 7,000 | 7,000 | **Cut from M0** (MR-10). |
| **D. Gate 0 census** | 30 | 100 | Small tables. |
| **E. H1, H2, H4 passes** | 0 | 100 | Local under export-first; a few credits each under in-warehouse. |
| **F. Getting the table out** | 50 | 1,900 | 50: in-warehouse, result grids only. 375: export-first at 1 credit/MB. 1,900: export-first at the documented Analyst rate. Decided at checkout (`STORAGE_DESIGNS.md`). |
| **Total, with the cut (no B2, no C)** | **6,000** | **11,600** | |
| Total with early-buyer groups | 9,100 | 16,200 | |
| Total with everything, `c07b` included | 16,100 | 23,200 | |

**Months of Analyst at 4,000 credits each:** with the cut, **two at the low end, three at the high end.** With the early-buyer groups, three to five.

## 3. Why the cut falls on `cluster_*`

* They serve H2 only. The Architect's crossover memo of the same day classes H2 as an observation edge, presumptively dead until a backtest says otherwise.
* They are 3,050–4,600 credits, the second-largest line.
* **As specified, the column does not measure what it is named for.** On 1,529 of 1,573 tokens the largest group of early buyers sharing a funder shares an exchange or a hub. With funder kind applied, the median group falls from 142 to between 3 and 9 depending on a threshold that has not been ruled (`CROSSOVER_MR11.md` §3). Building it for the full window before that ruling buys a number that will be redefined.
* Nothing else depends on it. H1 (the gate, RUG-A), H4 and the cost model are complete without it.

It stays cheap to add later: one query per chunk, already written and measured.

## 4. Kill criterion, against the measured numbers

As ratified: by **day 10** of the first Analyst month, (1) the event-derived table for the full window is built and exported for 3,000 credits or less, and (2) the heavy tier is measured on one month of the window; if it projects above **8,000** for the whole window, the comparison becomes the sampled window or a single Plus month, and no third month is bought blind.

Where that stands today:

* **Clause 2 is already answered for one era.** The whole heavy tier (B1 + B2 + B3) projects at **8,000–8,900** at June-2025 volume. That is over the line. With the cut it is **4,950–5,850**, under it.
* **So the comparison the criterion names is live now, not on day 10.** One Plus month is 25,000 credits, private queries (no public table), export at a fifth of Analyst's documented rate, and it finishes M0 in one month instead of two to four, with `cluster_*` and `c07b` included (23,200 at the high end). Analyst with the cut is two to three months.
* **The rule for choosing, to apply at checkout:** read both monthly prices with yearly billing switched off. If one Plus month costs no more than three Analyst months, the Plus month is the cheaper way to the same table and removes the public-table and export-rate questions. If it costs more, buy Analyst with the cut. The screen showed $349 for Plus and $65 for Analyst, both yearly-billed; neither monthly price has been read. **This is a recommendation to price it, not to press "Keep Plus".**
* **Two-month rule** (MR-10): no third Analyst month without a re-ruling on what the first two measured.
* **Day 10 now checks the era**, which the trial could not unless §6 is authorized: the first chunk built in the paid month should be a post-BOOST month, so the largest unknown is measured first.

## 5. What must be read at checkout

1. Analyst's **monthly** price with "Billed yearly" off. The $65 is the yearly-billed rate.
2. Plus's monthly price the same way, for the comparison in §4.
3. Whether overage past the allowance bills automatically. If it does, the runner gets a hard stop at the allowance.
4. **Analyst's export rate.** ≤ 3 credits/MB → export-first; the documented 10 → in-warehouse (`STORAGE_DESIGNS.md`).
5. Do not press "Keep Plus".

## 6. One use left for the expiring credits

477 spendable credits lapse on 2026-10-06. The single measurement that would most tighten this document is **one post-BOOST graduation day of the two large heavy queries**, 90–250 credits, both proven patterns. It would replace the ×1.5 in the high column with a number. It was not ordered, so it has not been run. **Requested: authorization for up to 250.**

## 7. Not settled

* **Era scaling of the heavy tier** (§6).
* **Organic-v2 in chunk form.** One readiness figure, not re-measured.
* **Three definitions awaiting signature** (`CROSSOVER_MR11.md` §6): the latency column's definition line, whether count columns carry raw and collapsed side by side, and the fan-out threshold with the launch-day fallback.
* **Columns that older events never filled.** Three found so far: `creator`, `is_buy` (with the trade amounts), `token_program`. Each has a replacement source. Every chunk's validation run null-counts its inputs per era (E35).
* **M0b (H3)** has no storage design on any self-serve plan.
