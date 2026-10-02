# Price paths after entry — burned post-BOOST week (runbook §1, MR-13)

**Date:** 2026-10-01 · **Week:** graduations 2026-08-10 → 08-16, chosen by seed and committed before the run (`RUNBOOK_ARCHITECT_2026-10-01.md`, commit `f3cfc07`) · **Tokens:** 7,225 stratum-P
**Spent:** 44.05 of the 50 allowed, plus 5.4 on the defect probes it triggered (below).
**Evidence:** `recon/out/run_pricepath_burned_week_*.json`. Query: `recon/sql/pricepath_burned_week.sql`.

**This week is burned.** It is excluded from every hypothesis evaluation. Everything here is descriptive: mid price to mid price, no fills, no costs, no gate.

## Two things wrong with this run, stated first

1. **A threshold went into Dune.** The query that ran computed "share of tokens reaching 2× entry" and the count of the H5 winner definition (24 h peak ≥ 3× entry and 7-day price ≥ entry) inside the query. The runbook's rule is that verdicts and thresholds never enter Dune. I had rewritten the query to return plain per-token numbers and count locally; the rewrite failed to apply and I ran the old text without checking. It was an unsaved ad-hoc query on a burned week, so nothing persists on Dune and no evaluation data was touched, but the rule was broken. The numbers are reported below because they exist.
2. **It cost about twice what it needed to.** The same failed rewrite would also have cut the event scans from six to two.

## Result, beside the June 2025 calibration week

| | Burned week, Aug 2026 (7,225) | | | June 2025 week (1,573) | | |
|---|---|---|---|---|---|---|
| Entry lag | 15 min | 60 min | 240 min | 15 min | 60 min | 240 min |
| Price at 7 d ÷ entry, median | 0.39 | 0.72 | 0.89 | 0.16 | 0.60 | 0.80 |
| Price at 7 d ÷ entry, mean | **0.50** | **0.66** | **0.78** | 0.54 | 0.73 | 0.84 |
| Share above entry at 7 d | 4.4% | 6.0% | 7.5% | 5.3% | 5.6% | 7.2% |
| Peak within 7 d ÷ entry, median | 1.15 | 1.03 | 1.00 | 1.36 | 1.23 | 1.18 |
| Share reaching 2× at any time | 21.4% | 14.0% | 10.4% | 29.8% | 22.6% | 20.7% |
| Peak within 24 h ÷ entry, median | 1.13 | 1.02 | 1.00 | 1.32 | 1.18 | 1.11 |
| Max drawdown within 7 d, median | 76% | 43% | 21% | 92% | 74% | 46% |
| Entry price ÷ graduation price, median | 0.33 | 0.15 | 0.10 | 0.55 | 0.12 | 0.07 |
| Real quote reserve at entry, median (SOL) | 9.1 | 2.4 | **1.4** | — | — | 23.4 |

H5 winner definition at G240: **63 of 7,225 (0.87%)**. Tokens whose 24 h peak reached 3× entry at G240: 285 (3.9%); 63 of those were still at or above entry after seven days.

## Reading

* **The picture holds in the current era.** A token bought at any registered lag and held seven days lost 22% to 50% on average before costs, and was above its entry 4–8% of the time. The mean is within seven points of the June week at every lag.
* **Post-BOOST tokens die faster and quieter.** At the 240-minute lag the median token's peak over the next seven days is its entry price, and its 75th-percentile seven-day ratio is 0.99: more than half barely move after that point. The upside tail is about half as frequent as in June 2025 (10% reach 2× against 21%).
* **Real depth is thin.** The median pool holds 1.4 SOL of withdrawable quote reserve four hours after graduation. The price is supported by the 17.58 SOL virtual reserve, which cannot be withdrawn. A $20 exit fits; the depth-floor rule will bind far more often than the one-day dry run suggested, and that dry run should not be relied on.
* **H5's base rate is under 1%.** About 60 winners per post-BOOST week. Over the roughly eight post-BOOST weeks outside the burned week that is a few hundred winners, enough to compare features but thin once split by era and marker.
* The entry-to-graduation ratio matches the one-day measurement of 2026-09-01 (0.32 / 0.15 / 0.10), which is a consistency check on the era-aware price.

## Defect found by this run: the buy variant cannot be read from Dune

The run returned a **negative** median buy cost (−120 bps). Cause, measured (`buyevent_variant_probe.sql`, 1.3 credits):

* **`ix_name` is NULL on every decoded PumpSwap buy event, in every era sampled** (June 2025, October 2025, April 2026, August 2026).
* The two quote fields swap roles by instruction variant. In 2025 `quote_amount_in` is always the smaller. In April 2026 it is the larger on 27% of buys; in August 2026 on 45%.
* Every query that chose gross and net by `ix_name` therefore took the same branch on every row.

**Fix, applied to the event-column generator:** gross is the larger of the two fields and net the smaller, whichever variant emitted the event. The amount entering the curve is the smaller in both variants, which is what the validated fill model uses.

**What it changes in earlier results**

* **Fee pricing per era (`M0_FEE_LEGS.md` Part 4)** used the `ix_name` form. Re-measured with the fix (`fees_per_era_v2.sql`, 1.8 credits), by side:

| Era | Buy cost, median bps | Sell cost, median bps | Creator fee, median (buy / sell) | Residual p90 (buy / sell) |
|---|---|---|---|---|
| 1 — to 2026-01-10 (2025-10-15) | 118.4 | 120.0 | 83 / 79 | 0 / 0 |
| 2 — to BOOST (2026-04-15) | 114.7 | 115.4 | 74 / 54 | 29 / 30 |
| 3 — post-BOOST (2026-09-01) | **99.1** | **119.5** | 21 / 34 | 39 / 73 |

  The headline stands: about 2.2–2.4% round trip in every era since October 2025. The post-BOOST buy leg is cheaper than the pooled 112 reported before. Swap totals are identical to the first run on all three days, so that run dropped nothing on those days; why the August week shows the swapped fields on most buys and those days do not is **not determined**, and is a null-count item for the first paid chunk.
* **This week's buy-cost column is invalid** and is not reported. The sell-cost median (125 bps) is unaffected.
* **Prices are affected only slightly.** The post-swap reserve after a buy used the wrong one of two fields that differ by about 1% of the trade size.
* **The June 2025 week is unaffected**: no buy had the swapped relation in 2025.

## Limits

* The virtual quote reserve is taken as one constant (17,584,505,500 lamports) for every pool born from 2026-07-21. It was read from ten post-BOOST swaps, all within 1,000 lamports of that. Whether it is the same on every pool, and whether the buy-and-burn changes it, is unverified. A check against pool accounts over RPC costs nothing and is in the runbook's day-one list.
* One week. Chosen by seed, so not picked for its result, but one week.
