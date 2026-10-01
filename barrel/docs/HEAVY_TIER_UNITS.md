# Heavy tier, measured — orders 3 and 4 of 2026-10-01

**Orders:** `RULINGS_2026-10-01.md` orders 3–4 (MR-10); crossover order 4 (`RULINGS_2026-10-01_CROSSOVER.md`, MR-11).
**Scope:** calibration week, graduations 2025-06-09 → 06-15, 1,573 stratum-P tokens (1,523 created within 3 days of graduating, the queries' scope limit). Pre-BOOST. One graduation day (241 tokens) was run first for every new query.
**Spent:** order 3 **57.3 credits** (4a itself 20.4 of its 90). Order 4 **284.6 of 350**, exports included. **Session total 341.9.**
**Balance:** 1,842.6 used of 2,500. **477.4 spendable above the 180 reserve.** Trial ends 2026-10-06.
**Evidence:** `recon/out/run_*_20261001T*.json` (cost, in-flight samples, rows). Rows that name wallets are in `barrel/private/out/` and are not in the repo.

---

## 1. Order 3 — corrected 4a

| Run | Credits | Result |
|---|---|---|
| Null-count probe of the query's inputs, one hour (`a3_inputs_nullcount_probe.sql`) | 2.5 | **`is_buy` is NULL on every bonding-curve trade event of June 2025** |
| Layout probe, 20 minutes in each of three eras (`tradeevent_layout_probe.sql`) | 2.9 (+0.4 for a cancelled first form) | see below |
| **Corrected 4a, one graduation day** (`a3_seta_day.sql`, v2) | **14.65** (cap 90) | 241 rows, sets populated |
| Week's 60 event columns regenerated (`pt_features_a_week.sql`) | 35.0 + 1.9 export | `creator` filled on 1,548 of 1,573 |

**A second empty column (E35 paid for itself on its first use).** On `pump_evt_tradeevent`, in June 2025 and October 2025, `is_buy`, `sol_amount`, `token_amount`, `virtual_sol_reserves` and `ix_name` are NULL on every row; only `user`, `mint` and the slot are filled. In September 2026 all are filled. The first 4a query filtered on `is_buy`, so even with the deployer fixed it would have found no creation-slot buyers and no bundles. The filter is removed, which loses nothing: a wallet that sells in the creation slot had to acquire in that slot. **Consequence beyond 4a:** bonding-curve trade direction and size cannot be read from this table before 2026. Anything that needs them uses the token ledger.

**A third: `token_program`** (schema column 9) is NULL on all 1,573 rows of the regenerated week, for the same reason. It will be taken from the mint-initialisation tables (B2 below), which have it. The regenerated file therefore has 59 columns with values, not 60.

**Why the first run cost 85 and this one 14.65.** The engine re-executes a `WITH` block at every place it is referenced. Version 1 referenced its SOL-transfer block and its ledger block several times each, so each large table was scanned once per reference. Version 2 references each once. Same output, one sixth of the cost. Every heavy query below is written to that rule, and where a block is referenced twice it is counted and stated.

**First `seta_sf_7d` distribution** (week, 1,523 tokens; set = creator + wallets the creator funded ± 24 h + creation-slot bundle wallets, restricted to members that ever held the token):

| | Tokens |
|---|---|
| Aligned set holds nothing at graduation + 240 min | 1,000 (66%) |
| Holds something | 523 (34%) |
| … sold none of it in 7 days | 342 |
| … sold 0–50% | 26 |
| … sold 50–99.9% | 19 |
| … **sold 99.9% or more** | **114** (22% of holders, 7.5% of all) |
| … bought more than it sold | 22 |

Share of supply held by the set at entry (`c07_g240`): median 0, 90th percentile 0.6%, 99th 26.7%, maximum 91%. Bundle at creation (`c08`): 43 tokens. No threshold is applied here; these are the inputs v1.2 sets one on.

**The set definition changed, and the old one was wrong.** Counted as written in readiness (creator plus every recipient of creator SOL within ± 24 h), the set has a median of 28 wallets and a minimum of 5, on every token. Those are fee accounts, tip accounts and rent for new accounts, which every creation pays. Restricted to members that ever held the token, and excluding the bonding curve, the median is **1** (the creator alone), the 90th percentile 3. 414 of 1,523 tokens have more than one member. The readiness sample's "118 funded wallets" and "1,128 funded wallets" are the unrestricted count.

---

## 2. Order 4 — the heavy columns, by query

| Query | Columns it feeds | One day | Week | Large-table days scanned (day → week) |
|---|---|---|---|---|
| **B1a** members + funding (`heavy_b1a_members_*.sql`) | c07 ×3, c08, `seta_*` ×4, `seta_lat_s`, `blk` | 44.47 | **76.66** | SOL transfers 6 → 13, four references |
| **B1b** holder concentration (`heavy_b1b_c06_*.sql`) | c06 ×3, supply at entry | 5.73 | **8.80** | token ledger 13 → 19 |
| **B2** authority history (`heavy_b2_auth_*.sql`) | c01, c02, `token_program` | 0.11 | **0.13** | small tables |
| **B3** early-buyer groups (`heavy_b3_cluster_*.sql`) | `cluster_*` ×4 | 38.67 | **61.84** | SOL transfers 9 → 15, three references |
| Fan-out histogram (`fanout_calibration_week.sql`) | calibration only | — | 11.50 | SOL transfers 12, one reference |
| **Sum of the four** | 20 of the 25 | 88.98 | **147.43** | |

Not rerun, priced from readiness: **organic-v2** (`org2_*`, 2 columns) 43.6 credits for a post-BOOST week. **c03, c04** apply only to Token-2022 mints; this week has none (all 1,523 are legacy SPL), so their cost here is zero and their era cost is unmeasured. **c05** is PASS by construction on P. **c11** is computed outside Dune (quarantine). **`u_codes`** is derived locally. **c07b** is out of M0.

Exports on top: 2.2 credits for the 4,411 member rows; **29 credits for the 113,709 group rows of B3**. The B3 export was a measurement convenience. In the build, the group table is reduced to one row per token inside the query.

### Both scaling models, as ordered

| | Per token | Per day scanned |
|---|---|---|
| Unit (week) | **0.094 credits per token** (B1a 0.049, B1b 0.006, B2 0.0001, B3 0.039) | B1a **5.9**, B3 **4.1**, B1b **0.46** credits per large-table day |
| Marginal, day → week | — | B1a **4.6**, B3 **3.9**, B1b **0.5** per added day |
| Predicts the week from the day | 581 (tokens × 6.5) | 169 (B1a 96, B3 64, B1b 8.4) |
| **Actual week** | **147.4** | **147.4** |
| Full window, 180,543 tokens / 610 days | 16,900 | **5,500** in one pass; **7,100 in 20 monthly chunks** (each chunk re-scans its lookback) |

**The per-token model is rejected by the data: it over-predicts the week by 4×.** Cost follows days of large table scanned, as it did for the event columns. Monthly chunks are the ratified plan, so **7,100 is the planning figure for these four queries**, about 350 per chunk (B1a 178, B3 151, B1b 21, B2 under 1).

### What this number does not cover
* **Era.** Both points are June 2025. Post-BOOST days carry about 3× the swap rows; what SOL-transfer volume does over the window is not measured. One post-BOOST graduation day of B1a would measure it (see §4).
* **Organic-v2.** 43.6 per week in readiness, with a 60-day lookback. Not re-measured in chunk form; carried as 900–1,800.
* **Funding lookback.** Funders are searched only inside the scanned window (4 days before the first graduation day). Lengthening it raises the cost one-for-one in days scanned.

---

## 3. What the runner's samples show (order 1 follow-up)

Every run now records Dune's in-flight cost counter at each poll. It is **not continuous**. On the B1a week run it read 8.25 from second 38 to second 173, then 64.8. On the day run it went from 12.9 to 35.1 between consecutive changes. **A cap enforced by cancellation can be overshot by tens of credits in one step**, whatever the poll interval. The hard cap bounds what a *slow* overrun costs; the protection against a fast one is the rule that already exists: a new pattern runs at one-partition scope first, with the cap set where losing it is acceptable. Every run that completed today finished under its cap; one probe was cancelled at 5.0 against a cap of 4 and billed 0.4.

---

## 4. One measurement left open, for a ruling

The heavy tier has been measured in one era. A single post-BOOST graduation day of B1a and B3 (same queries, proven patterns) would say whether the 7,100 holds, doubles or triples in the era that holds most of the window's volume. Expected 90–250 credits. **477 are spendable and expire on 2026-10-06.** It is outside the 350 ordered for order 4, so it has not been run. Requested: authorization for up to 250.
